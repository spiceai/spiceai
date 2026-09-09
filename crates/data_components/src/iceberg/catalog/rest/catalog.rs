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
    TableIdent, table::Table,
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

    /// Drop a table from the catalog and delete the underlying table data.
    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
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

/// Guards the SigV4 signing middleware the `spiceai/iceberg-rust` fork adds to its
/// REST catalog client, which is what lets a Glue-backed Iceberg catalog
/// authenticate at all.
///
/// The parameter wiring is compile-guarded — `crates/runtime/src/catalogconnector/iceberg.rs`
/// sets `rest.sigv4-enabled` — but that only proves the flag reaches the client. If
/// a re-cut drops the middleware while keeping the flag, every request goes out
/// unsigned and Glue rejects it, which reads as a credentials problem rather than a
/// lost patch.
///
/// Signed against a local stub with static credentials, so this needs no AWS
/// account: what is asserted is the request Spice puts on the wire.
/// `docs/dev/fork_patches.md` is the ledger this guard is named in.
#[cfg(test)]
mod sigv4_signing {
    use std::collections::HashMap;

    use iceberg::{Catalog as _, CatalogBuilder};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::RestCatalog;

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const REGION: &str = "ap-northeast-2";

    /// A stub REST catalog that answers the two calls a `list_namespaces` makes: the
    /// config fetch the client always issues first, and the listing itself.
    async fn stub_catalog() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {},
                "overrides": {},
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "namespaces": [] })),
            )
            .mount(&server)
            .await;
        server
    }

    /// List namespaces through a catalog built from `props`, and return one entry per
    /// request the stub saw — `None` where that request carried no `Authorization`.
    ///
    /// One entry per *request*, not per header: filtering the unsigned ones out would
    /// let a middleware that signs only some requests pass the assertion below, since
    /// the requests it skipped would simply not appear.
    async fn authorization_headers(
        server: &MockServer,
        props: HashMap<String, String>,
    ) -> Vec<Option<String>> {
        let catalog = RestCatalog::new(
            RestCatalogBuilder::default()
                .load("rest", props)
                .await
                .expect("the REST catalog is configured"),
        );
        catalog
            .list_namespaces(None)
            .await
            .expect("the stub answers the listing");

        server
            .received_requests()
            .await
            .expect("the stub records what it received")
            .iter()
            .map(|request| {
                request
                    .headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(ToString::to_string)
            })
            .collect()
    }

    fn props(server: &MockServer, sigv4: bool) -> HashMap<String, String> {
        let mut props = HashMap::from([("uri".to_string(), server.uri())]);
        if sigv4 {
            props.extend([
                ("rest.sigv4-enabled".to_string(), "true".to_string()),
                ("rest.signing-region".to_string(), REGION.to_string()),
                ("rest.signing-name".to_string(), "glue".to_string()),
                ("rest.access-key-id".to_string(), ACCESS_KEY.to_string()),
                (
                    "rest.secret-access-key".to_string(),
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                ),
            ]);
        }
        props
    }

    #[tokio::test]
    async fn a_sigv4_catalog_signs_every_request_it_sends() {
        let server = stub_catalog().await;
        let requests = authorization_headers(&server, props(&server, true)).await;

        assert!(
            !requests.is_empty(),
            "the stub saw no request at all, so this asserts nothing"
        );
        // Every request, not merely every signature: a catalog that signs the config
        // fetch and not the listing authenticates for exactly as long as it takes to
        // reach the first real call.
        let unsigned = requests.iter().filter(|header| header.is_none()).count();
        assert_eq!(
            unsigned,
            0,
            "{unsigned} of {} requests carried no Authorization header, so the signing middleware is missing or applied to only some calls",
            requests.len()
        );
        for header in requests.iter().flatten() {
            assert!(
                header.starts_with("AWS4-HMAC-SHA256 "),
                "expected a SigV4 signature, got: {header}"
            );
            // The credential scope is what Glue checks after the signature: the wrong
            // region or service name is rejected the same way an unsigned request is.
            assert!(
                header.contains(&format!("Credential={ACCESS_KEY}/")),
                "the signature does not name the configured key: {header}"
            );
            assert!(
                header.contains(&format!("/{REGION}/glue/aws4_request")),
                "the signature's credential scope is not the configured region and service: {header}"
            );
        }
    }

    /// The control: without the flag the same client sends nothing, so the assertion
    /// above is reading the middleware rather than a header the client always sets.
    #[tokio::test]
    async fn a_catalog_without_sigv4_sends_no_signature() {
        let server = stub_catalog().await;
        let requests = authorization_headers(&server, props(&server, false)).await;
        assert!(!requests.is_empty(), "the stub saw no request at all");
        assert!(
            requests.iter().all(Option::is_none),
            "a catalog with no SigV4 configured signed a request anyway: {requests:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_datafusion::IcebergTableProvider;
    use iceberg_storage_opendal::OpenDalStorageFactory;
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
                .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                }))
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

        let catalog = Arc::new(catalog) as Arc<dyn Catalog>;
        let df_table_provider = IcebergTableProvider::try_new(
            catalog,
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
