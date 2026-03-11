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
    Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation,
    TableIdent, io::CustomAwsCredentialLoader, table::Table,
};
use iceberg_catalog_rest::RestCatalog as IcebergRestCatalog;

#[derive(Debug)]
pub struct RestCatalog {
    inner: IcebergRestCatalog,
}

impl RestCatalog {
    #[must_use]
    pub fn new(inner: IcebergRestCatalog) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn with_file_io_extension(
        self,
        custom_credential_loader: CustomAwsCredentialLoader,
    ) -> Self {
        Self {
            inner: self.inner.with_file_io_extension(custom_credential_loader),
        }
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    /// Register an existing table to the catalog.
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

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
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
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
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    /// Update a table to the catalog.
    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_datafusion::IcebergTableProvider;
    use std::sync::Arc;

    use super::*;

    /// Comment the `#[ignore]` and run this test with `cargo test -p data_components --lib -- iceberg::catalog --nocapture`.
    ///
    /// Pre-requisites:
    /// Follow the guide at <https://iceberg.apache.org/spark-quickstart/> to spin up a local Iceberg catalog/Minio & Spark cluster.
    /// In the Python notebook that gets started at <http://localhost:8888>, load the `Iceberg - Getting Started.ipynb` notebook.
    /// Run the first 5 cells to create the `nyc.taxis` table.
    #[tokio::test]
    #[ignore = "requires local minio and spark cluster"]
    async fn test_rest_catalog() {
        let catalog = RestCatalog::new(
            RestCatalogBuilder::default()
                .load(
                    "rest",
                    HashMap::from([
                        ("uri".to_string(), "http://localhost:8181".to_string()),
                        (
                            "s3.endpoint".to_string(),
                            "http://localhost:9000".to_string(),
                        ),
                        ("s3.access-key-id".to_string(), "admin".to_string()),
                        ("s3.secret-access-key".to_string(), "password".to_string()),
                        ("s3.region".to_string(), "us-east-1".to_string()),
                    ]),
                )
                .await
                .expect("valid catalog"),
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

        let df_table_provider = IcebergTableProvider::try_new(
            Arc::new(catalog),
            NamespaceIdent::new("nyc".to_string()),
            "taxis".to_string(),
        )
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
