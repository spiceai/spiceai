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

//! Implementation of an Iceberg REST API Catalog client that knows how to load Spice.ai and/or Iceberg tables.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::{
    table::Table, Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent,
    Result as IcebergResult, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog as IcebergRestCatalog, RestCatalogConfig};

#[derive(Debug)]
pub struct RestCatalog {
    inner: IcebergRestCatalog,
}

impl RestCatalog {
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(catalog_config: RestCatalogConfig) -> Self {
        Self {
            inner: IcebergRestCatalog::new(catalog_config),
        }
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Create namespace is not implemented",
        ));
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Update namespace is not implemented",
        ));
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Drop namespace is not implemented",
        ));
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> IcebergResult<Table> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Create table is not implemented",
        ));
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Drop table is not implemented",
        ));
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Rename table is not implemented",
        ));
    }

    /// Update a table to the catalog.
    async fn update_table(&self, _commit: TableCommit) -> IcebergResult<Table> {
        return Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Update table is not implemented",
        ));
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use iceberg_datafusion::IcebergTableProvider;
    use std::sync::Arc;

    use super::*;

    /// Comment the `#[ignore]` and run this test with `cargo test -p data_components --lib -- iceberg::catalog --nocapture`.
    ///
    /// Pre-requisites:
    /// Follow the guide at https://iceberg.apache.org/spark-quickstart/ to spin up a local Iceberg catalog/Minio & Spark cluster.
    /// In the Python notebook that gets started at http://localhost:8888, load the `Iceberg - Getting Started.ipynb` notebook.
    /// Run the first 5 cells to create the `nyc.taxis` table.
    #[tokio::test]
    #[ignore]
    async fn test_rest_catalog() {
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .props(HashMap::from([
                    (
                        "s3.endpoint".to_string(),
                        "http://localhost:9000".to_string(),
                    ),
                    ("s3.access-key-id".to_string(), "admin".to_string()),
                    ("s3.secret-access-key".to_string(), "password".to_string()),
                    ("s3.region".to_string(), "us-east-1".to_string()),
                ]))
                .build(),
        );

        let namespaces = catalog.list_namespaces(None).await;
        println!("{namespaces:?}");

        let namespace = catalog
            .get_namespace(&NamespaceIdent::new("nyc".to_string()))
            .await;
        println!("{namespace:?}");

        let tables = catalog
            .list_tables(&NamespaceIdent::new("nyc".to_string()))
            .await;
        println!("{tables:?}");

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("nyc".to_string()),
                "taxis".to_string(),
            ))
            .await
            .expect("Failed to load table");
        println!("{table:?}");

        let df_table_provider = IcebergTableProvider::try_new_from_table(table)
            .await
            .expect("Failed to create table provider");

        let ctx = SessionContext::new();
        ctx.register_table("ice_ice_baby", Arc::new(df_table_provider))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT * FROM ice_ice_baby LIMIT 10")
            .await
            .expect("Failed to execute query");
        df.show().await.expect("Failed to show");
    }
}
