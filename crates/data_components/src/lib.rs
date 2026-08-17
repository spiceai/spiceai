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
use std::{collections::HashMap, error::Error, hash::BuildHasher, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::CatalogProvider,
    common::{Statistics, stats::Precision},
    datasource::TableProvider,
    sql::TableReference,
};
use datafusion_federation::FederatedTableProviderAdaptor;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// Canonical Arrow metadata keys, re-exported from `arrow_tools` where they are
/// defined, so connectors keep reaching them through this crate.
pub use arrow_tools::metadata_keys::{
    CLUSTERING_KEY_METADATA_KEY, CLUSTERING_METADATA_KEY, DESCRIPTION_METADATA_KEY,
    FOREIGN_KEYS_METADATA_KEY, INFERRED_COLUMN_STATS_METADATA_KEY, INFERRED_INDEXES_METADATA_KEY,
    INFERRED_PRIMARY_KEY_METADATA_KEY, INFERRED_ROW_COUNT_METADATA_KEY,
    INFERRED_SHARD_KEY_METADATA_KEY, INFERRED_SORT_COLUMNS_METADATA_KEY,
    INFERRED_TABLE_BYTES_METADATA_KEY, PARTITION_METADATA_KEY, SOURCE_TYPE_METADATA_KEY,
};

/// Metadata to merge into fields, keyed by field name.
pub type FieldMetadata = HashMap<String, HashMap<String, String>>;

#[cfg(feature = "adbc")]
pub mod adbc_helpers;
pub mod arrow;
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
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "federation")]
pub mod federation;
pub mod flight;
#[cfg(feature = "flightsql")]
pub mod flightsql;
pub mod function_support;
pub mod iceberg;
pub mod inferred_schema;
#[cfg(any(feature = "debezium", feature = "kafka"))]
pub mod kafka;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "mysql")]
pub mod mysql_replication;
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
/// Connector-agnostic schema projection (JSON nesting). The core lives in the
/// `datafusion-table-providers` fork so providers defined there (`MongoDB`) can
/// reuse it; re-exported here for the in-repo connectors (`DynamoDB`, Debezium).
pub use datafusion_table_providers::schema_projection;
pub mod sql_expr;

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

pub mod catalog_filter;
pub mod key_filter;
pub mod pk_filter_expr;
pub mod rate_limit;

pub mod cdc;
pub mod delete;
pub mod http;
pub mod index_maintenance;
pub mod object;
pub mod poly;
pub mod update;

/// A layer that merges additional metadata into the Arrow schema.
///
/// Declares only what it changes — the schema, which is the original with
/// `extra_metadata` merged in, and the statistics it can infer from that
/// metadata. Everything else is the table beneath it.
pub struct MetadataEnrichedTableProvider {
    schema: SchemaRef,
}

impl MetadataEnrichedTableProvider {
    /// Wrap `inner`, merging `extra_metadata` into its schema-level metadata.
    ///
    /// Keys in `extra_metadata` will overwrite any pre-existing schema metadata with the same key.
    #[must_use]
    pub fn new<S>(
        inner: &Arc<dyn TableProvider>,
        extra_metadata: HashMap<String, String, S>,
    ) -> Self
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
        inner: &Arc<dyn TableProvider>,
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
        Self { schema }
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

    if let Some(adaptor) = provider.downcast_ref::<FederatedTableProviderAdaptor>() {
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

    SpiceTable::over(
        Arc::new(MetadataEnrichedTableProvider::new_with_field_metadata(
            &provider,
            extra_metadata,
            &field_metadata,
        )),
        provider,
    ) as Arc<dyn TableProvider>
}

impl std::fmt::Debug for MetadataEnrichedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataEnrichedTableProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableLayer for MetadataEnrichedTableProvider {
    /// Injects spicepod table/column metadata into the schema and carries no
    /// read, CDC, source or index semantics of its own — so every walk but the
    /// write walk sees past it, matching the write pass-through it does not have.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind
        // for this layer without anyone deciding what it should say.
        match walk {
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::RetentionDelete
            | LayerWalk::Index => Some(below),
            LayerWalk::Write => None,
        }
    }

    fn schema(&self, _below: &Arc<dyn TableProvider>) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self, below: &Arc<dyn TableProvider>) -> Option<Statistics> {
        // Surface the inferred rough table size as table statistics so the query
        // optimizer, acceleration sizing, and observability can use it. Prefer the
        // provider beneath where it has them (they may be exact or cheaper),
        // filling only the row-count / byte-size fields it leaves `Absent` with
        // the inferred estimate.
        match (inferred_statistics(&self.schema), below.statistics()) {
            (Some(inferred), Some(mut inner)) => {
                if matches!(inner.num_rows, Precision::Absent) {
                    inner.num_rows = inferred.num_rows;
                }
                if matches!(inner.total_byte_size, Precision::Absent) {
                    inner.total_byte_size = inferred.total_byte_size;
                }
                Some(inner)
            }
            (inferred, inner) => inferred.or(inner),
        }
    }
}

/// Build `DataFusion` table statistics from the rough row-count / byte-size keys in
/// `schema`'s metadata, if either was inferred (see [`inferred_schema`]). Column
/// statistics are left unknown. Returns `None` when no size was inferred.
fn inferred_statistics(schema: &SchemaRef) -> Option<Statistics> {
    let inferred = inferred_schema::InferredSchema::from_metadata(schema.metadata());
    if inferred.row_count.is_none() && inferred.table_bytes.is_none() {
        return None;
    }

    let mut stats = Statistics::new_unknown(schema);
    // Leave a field unset (rather than saturating to `usize::MAX`) when the inferred
    // u64 doesn't fit `usize` — a wrong, huge estimate is worse than no estimate.
    if let Some(rows) = inferred.row_count.and_then(|r| usize::try_from(r).ok()) {
        stats = stats.with_num_rows(Precision::Inexact(rows));
    }
    if let Some(bytes) = inferred.table_bytes.and_then(|b| usize::try_from(b).ok()) {
        stats = stats.with_total_byte_size(Precision::Inexact(bytes));
    }
    Some(stats)
}

#[async_trait]
pub trait Read: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;

    /// A provider for a table whose schema the caller has already resolved.
    ///
    /// Lets a caller holding many schemas at once -- a catalog that resolved a
    /// whole namespace in one query, say -- build providers without a round trip
    /// per table.
    ///
    /// The default delegates to [`Read::table_provider`], discarding `schema`
    /// and resolving it from the source instead: correct, and as costly as not
    /// having asked. An implementation that overrides this must return a
    /// provider indistinguishable from [`Read::table_provider`]'s -- same
    /// wrappers, same pushdown -- since a table that plans differently depending
    /// on how it was discovered is a bug the caller cannot see.
    async fn table_provider_with_schema(
        &self,
        table_reference: TableReference,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        self.table_provider(table_reference).await
    }
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

/// A [`CatalogProvider`] that periodically refreshes its contents from a remote
/// catalog by polling the wrapped [`RefreshableCatalogProvider`].
///
/// This is a *transparent* wrapper: every catalog registered through a
/// `RefreshableCatalogProvider` is wrapped in one of these. Concrete-type
/// detection (e.g. "is this catalog Cayenne-backed?") must therefore peel the
/// wrapper via [`RefreshingCatalogProvider::inner_catalog`] before downcasting.
///
/// `DataFusion` 54 removed `CatalogProvider::as_any`, which this wrapper used to
/// delegate to its inner provider so that `downcast_ref::<ConcreteProvider>()`
/// transparently saw through it. The `Any`-based `downcast_ref` that replaced it
/// can only ever resolve to the wrapper's own type, so callers must peel
/// explicitly — see [`RefreshingCatalogProvider::inner_catalog`].
#[derive(Debug)]
pub struct RefreshingCatalogProvider {
    /// Named by the refresh failure this logs. A runtime serving several
    /// catalogs refreshes them all on the same loop, so a message that does not
    /// name one says only that something, somewhere, is out of date.
    catalog_name: String,
    inner: Arc<dyn RefreshableCatalogProvider>,
    refresh_task: Option<tokio::task::JoinHandle<()>>,
}

impl RefreshingCatalogProvider {
    #[must_use]
    pub fn new(catalog_name: String, inner: Arc<dyn RefreshableCatalogProvider>) -> Self {
        Self {
            catalog_name,
            inner,
            refresh_task: None,
        }
    }

    /// Returns the wrapped catalog provider.
    ///
    /// Catalog-type detection must peel this wrapper via this accessor to reach
    /// the underlying provider (e.g. a `CayenneCatalogProvider`); see the
    /// type-level documentation for why.
    #[must_use]
    pub fn inner_catalog(&self) -> &dyn CatalogProvider {
        self.inner.as_ref()
    }

    /// Spawns the background refresh loop and returns the started provider.
    #[must_use]
    pub fn start_refresh(mut self, interval: Option<std::time::Duration>) -> Self {
        let interval = interval.unwrap_or(std::time::Duration::from_mins(1));
        let inner = Arc::clone(&self.inner);
        let catalog_name = self.catalog_name.clone();
        let retry_secs = interval.as_secs();
        self.refresh_task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = inner.refresh().await {
                    tracing::error!(
                        "Catalog {catalog_name} is still serving the tables from its last successful refresh, which may now be out of date: a table added, renamed or dropped in the source since then is not reflected. It is retried in {retry_secs}s. Failed to refresh it: {e}"
                    );
                }
            }
        }));
        self
    }
}

#[deny(clippy::missing_trait_methods)]
impl CatalogProvider for RefreshingCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        self.inner.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn datafusion::catalog::SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::SchemaProvider>>> {
        self.inner.deregister_schema(name, cascade)
    }
}

impl Drop for RefreshingCatalogProvider {
    fn drop(&mut self) {
        if let Some(task) = self.refresh_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::catalog::Session;
    use datafusion::datasource::TableType;
    use datafusion::error::DataFusionError;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::{LogicalPlan, TableSource};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::Expr;
    use datafusion_federation::{
        FederatedTableProviderAdaptor, FederatedTableSource, FederationAnalyzerForLogicalPlan,
        FederationProvider,
    };

    #[test]
    fn inferred_statistics_from_size_metadata() {
        let metadata = HashMap::from([
            (
                INFERRED_ROW_COUNT_METADATA_KEY.to_string(),
                "1000".to_string(),
            ),
            (
                INFERRED_TABLE_BYTES_METADATA_KEY.to_string(),
                "2048".to_string(),
            ),
        ]);
        let schema: SchemaRef = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            metadata,
        ));

        let stats = inferred_statistics(&schema).expect("inferred size yields statistics");
        assert_eq!(stats.num_rows, Precision::Inexact(1000));
        assert_eq!(stats.total_byte_size, Precision::Inexact(2048));
    }

    #[test]
    fn no_inferred_statistics_without_size_metadata() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(inferred_statistics(&schema).is_none());
    }

    #[derive(Debug)]
    struct TestFederationProvider;

    impl FederationProvider for TestFederationProvider {
        fn name(&self) -> &'static str {
            "test"
        }

        fn compute_context(&self) -> Option<String> {
            Some("test-context".to_string())
        }

        fn analyzer(&self, _plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
            None
        }
    }

    #[derive(Debug)]
    struct TestFederatedSource {
        schema: SchemaRef,
    }

    impl TableSource for TestFederatedSource {
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
