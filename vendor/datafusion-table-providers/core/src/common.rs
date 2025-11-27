use std::{any::Any, sync::Arc};

use crate::sql::db_connection_pool::dbconnection::{get_schemas, get_tables};
use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::SqlTable;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    sql::TableReference,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Pool<T, P> = Arc<dyn DbConnectionPool<T, P> + Send + Sync>;

#[derive(Debug)]
pub struct DatabaseCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl DatabaseCatalogProvider {
    pub async fn try_new<T: 'static, P: 'static>(pool: Pool<T, P>) -> Result<Self> {
        let conn = pool.connect().await?;

        let schemas = get_schemas(conn).await?;
        let schema_map = DashMap::new();

        for schema in schemas {
            let provider = DatabaseSchemaProvider::try_new(schema.clone(), pool.clone()).await?;
            schema_map.insert(schema, Arc::new(provider) as Arc<dyn SchemaProvider>);
        }

        Ok(Self {
            schemas: schema_map,
        })
    }
}

impl CatalogProvider for DatabaseCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|s| s.clone())
    }
}

pub struct DatabaseSchemaProvider<T, P> {
    name: String,
    tables: Vec<String>,
    pool: Pool<T, P>,
}

impl<T, P> std::fmt::Debug for DatabaseSchemaProvider<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseSchemaProvider {{ name: {:?} }}", self.name)
    }
}

impl<T, P: 'static> DatabaseSchemaProvider<T, P> {
    pub async fn try_new(name: String, pool: Pool<T, P>) -> Result<Self> {
        let conn = pool.connect().await?;
        let tables = get_tables(conn, &name).await?;

        Ok(Self { name, tables, pool })
    }
}

#[async_trait]
impl<T: 'static, P: 'static> SchemaProvider for DatabaseSchemaProvider<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, table: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(table) {
            SqlTable::new(
                "db", // Generic name for database schema provider
                &self.pool,
                TableReference::partial(self.name.clone(), table.to_string()),
                None, // No specific engine for generic provider
            )
            .await
            .map(|v| Some(Arc::new(v) as Arc<dyn TableProvider>))
            .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains(&name.to_string())
    }
}
