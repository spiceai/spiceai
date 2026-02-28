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

//! `PostgreSQL` catalog provider implementation.
//!
//! Discovers schemas and tables in a `PostgreSQL` database using
//! `information_schema` queries and provides them as `DataFusion` catalog/schema providers.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use globset::GlobSet;
use snafu::prelude::*;

use crate::{Read, RefreshableCatalogProvider};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get connection from PostgreSQL pool: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to execute PostgreSQL query: {source}"))]
    QueryFailed { source: tokio_postgres::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "pg_toast"];

/// A catalog provider for `PostgreSQL` that discovers schemas and tables
/// by querying `information_schema`.
pub struct PostgresCatalogProvider {
    pool: Arc<PostgresConnectionPool>,
    table_creator: Arc<dyn Read>,
    schemas: RwLock<HashMap<String, Arc<PostgresSchemaProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for PostgresCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl PostgresCatalogProvider {
    #[must_use]
    pub fn new(
        pool: Arc<PostgresConnectionPool>,
        table_creator: Arc<dyn Read>,
        include: Option<GlobSet>,
    ) -> Self {
        Self {
            pool,
            table_creator,
            schemas: RwLock::new(HashMap::new()),
            include: include.map(Arc::new),
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let schema_names = self.list_schemas().await?;

        let mut schemas = HashMap::new();
        for schema_name in schema_names {
            let schema_provider = PostgresSchemaProvider::new(
                Arc::clone(&self.pool),
                schema_name.clone(),
                Arc::clone(&self.table_creator),
                self.include.clone(),
            );
            schema_provider.refresh_tables().await?;
            schemas.insert(schema_name, Arc::new(schema_provider));
        }

        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
        }

        Ok(())
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        let rows = conn
            .conn
            .query(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
                &[],
            )
            .await
            .context(QueryFailedSnafu)?;

        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                if SYSTEM_SCHEMAS.contains(&name.as_str()) || name.starts_with("pg_temp") {
                    None
                } else {
                    Some(name)
                }
            })
            .collect();

        Ok(names)
    }
}

impl CatalogProvider for PostgresCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

#[async_trait]
impl RefreshableCatalogProvider for PostgresCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for `PostgreSQL` that discovers tables within a schema.
pub struct PostgresSchemaProvider {
    pool: Arc<PostgresConnectionPool>,
    schema_name: String,
    table_creator: Arc<dyn Read>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for PostgresSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl PostgresSchemaProvider {
    #[must_use]
    pub fn new(
        pool: Arc<PostgresConnectionPool>,
        schema_name: String,
        table_creator: Arc<dyn Read>,
        include: Option<Arc<GlobSet>>,
    ) -> Self {
        Self {
            pool,
            schema_name,
            table_creator,
            tables: RwLock::new(HashMap::new()),
            include,
        }
    }

    async fn refresh_tables(&self) -> Result<()> {
        let table_names = self.list_tables().await?;

        let tables = build_table_providers_for_schema(
            &self.schema_name,
            table_names,
            &self.table_creator,
            self.include.as_deref(),
        )
        .await;

        {
            let mut guard = match self.tables.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = tables;
        }

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        let rows = conn
            .conn
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 \
                 AND table_type IN ('BASE TABLE', 'VIEW') \
                 ORDER BY table_name",
                &[&self.schema_name],
            )
            .await
            .context(QueryFailedSnafu)?;

        let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(names)
    }
}

fn is_table_included(schema_name: &str, table_name: &str, include: Option<&GlobSet>) -> bool {
    let schema_with_table = format!("{schema_name}.{table_name}");
    include.is_none_or(|globset| globset.is_match(&schema_with_table))
}

async fn build_table_providers_for_schema(
    schema_name: &str,
    table_names: Vec<String>,
    table_creator: &Arc<dyn Read>,
    include: Option<&GlobSet>,
) -> HashMap<String, Arc<dyn TableProvider>> {
    let mut tables = HashMap::new();

    for table_name in table_names {
        let schema_with_table = format!("{schema_name}.{table_name}");
        if !is_table_included(schema_name, &table_name, include) {
            tracing::debug!("Table {schema_with_table} is not included, skipping");
            continue;
        }

        let table_ref = TableReference::partial(schema_name.to_owned(), table_name.clone());

        match table_creator.table_provider(table_ref).await {
            Ok(provider) => {
                tables.insert(table_name, provider);
            }
            Err(e) => {
                tracing::warn!(
                    schema = %schema_name,
                    table = %table_name,
                    error = %e,
                    "Failed to create table provider for PostgreSQL table {schema_with_table}, skipping"
                );
            }
        }
    }

    tables
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        Ok(guard.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{build_table_providers_for_schema, is_table_included};
    use crate::Read;
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::Expr;
    use datafusion::sql::TableReference;
    use globset::{Glob, GlobSetBuilder};
    use std::any::Any;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct MockTableProvider;

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(arrow::datatypes::Schema::empty())
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
            unimplemented!("Not needed for tests")
        }
    }

    #[derive(Debug)]
    struct MockRead {
        fail_tables: HashSet<String>,
        seen_tables: Mutex<Vec<String>>,
    }

    impl MockRead {
        fn new(fail_tables: HashSet<String>) -> Self {
            Self {
                fail_tables,
                seen_tables: Mutex::new(Vec::new()),
            }
        }

        fn seen_tables(&self) -> Vec<String> {
            self.seen_tables
                .lock()
                .expect("seen_tables mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl Read for MockRead {
        async fn table_provider(
            &self,
            table_reference: TableReference,
        ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
            let (schema, table) = match table_reference {
                TableReference::Partial { schema, table } => {
                    (schema.to_string(), table.to_string())
                }
                _ => return Err("expected partial table reference".into()),
            };

            let full_name = format!("{schema}.{table}");
            self.seen_tables
                .lock()
                .expect("seen_tables mutex should not be poisoned")
                .push(full_name.clone());

            if self.fail_tables.contains(&full_name) {
                return Err("simulated table provider creation failure".into());
            }

            Ok(Arc::new(MockTableProvider))
        }
    }

    fn make_include(patterns: &[&str]) -> Arc<globset::GlobSet> {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(Glob::new(pattern).expect("glob pattern should parse"));
        }
        Arc::new(builder.build().expect("glob set should build"))
    }

    #[test]
    fn test_is_table_included_with_glob_filter() {
        let include = make_include(&["public.orders"]);
        assert!(is_table_included("public", "orders", Some(&include)));
        assert!(!is_table_included("public", "lineitem", Some(&include)));
    }

    #[tokio::test]
    async fn test_build_table_providers_applies_include_filter_before_factory() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let include = make_include(&["public.orders"]);
        let table_creator: Arc<dyn Read> = read.clone();

        let tables = build_table_providers_for_schema(
            "public",
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            Some(&include),
        )
        .await;

        assert_eq!(tables.len(), 1);
        assert!(tables.contains_key("orders"));
        assert_eq!(read.seen_tables(), vec!["public.orders".to_string()]);
    }

    #[tokio::test]
    async fn test_build_table_providers_skips_failed_table_provider_creation() {
        let mut fail_tables = HashSet::new();
        fail_tables.insert("public.orders".to_string());
        let read = Arc::new(MockRead::new(fail_tables));
        let table_creator: Arc<dyn Read> = read.clone();

        let tables = build_table_providers_for_schema(
            "public",
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            None,
        )
        .await;

        assert_eq!(
            tables.keys().cloned().collect::<HashSet<String>>(),
            HashSet::from(["lineitem".to_string()])
        );
        assert_eq!(
            read.seen_tables().into_iter().collect::<HashSet<String>>(),
            HashSet::from(["public.orders".to_string(), "public.lineitem".to_string()])
        );
    }

    #[tokio::test]
    async fn test_build_table_providers_returns_empty_when_all_factory_calls_fail() {
        let fail_tables = HashSet::from(["public.orders".to_string(), "public.lineitem".to_string()]);
        let read = Arc::new(MockRead::new(fail_tables));
        let table_creator: Arc<dyn Read> = read.clone();

        let tables: HashMap<String, Arc<dyn TableProvider>> = build_table_providers_for_schema(
            "public",
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            None,
        )
        .await;

        assert!(tables.is_empty(), "all failing tables should be skipped");
    }
}
