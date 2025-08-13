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

#[cfg(feature = "test_hadoop_catalog_docker")]
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "test_hadoop_catalog_docker")]
use std::sync::RwLock;

use arrow_array::RecordBatch;
#[cfg(feature = "test_hadoop_catalog_docker")]
use ctor::{ctor, dtor};
use data_components::iceberg::catalog::hadoop::{HadoopCatalog, HadoopCatalogBuilder};
use futures::TryStreamExt;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, NamespaceIdent};
#[cfg(feature = "test_hadoop_catalog_docker")]
use iceberg_test_utils::docker::DockerCompose;
#[cfg(feature = "test_hadoop_catalog_docker")]
use iceberg_test_utils::normalize_test_name;

#[cfg(feature = "test_hadoop_catalog_docker")]
const MINIO_PORT: u16 = 9000;

#[cfg(feature = "test_hadoop_catalog_docker")]
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[cfg(feature = "test_hadoop_catalog_docker")]
fn get_file_hadoop_catalog() -> HadoopCatalogBuilder {
    HadoopCatalogBuilder::default().with_warehouse_root("file:///tmp/hadoop_warehouse")
}

#[allow(clippy::expect_used)]
fn get_s3a_hadoop_catalog() -> HadoopCatalogBuilder {
    #[cfg(not(feature = "test_hadoop_catalog_docker"))]
    let minio_endpoint = std::env::var("MINIO_ENDPOINT")
        .expect("Should have MINIO_ENDPOINT environment variable set");

    #[cfg(feature = "test_hadoop_catalog_docker")]
    let minio_endpoint = {
        let guard = DOCKER_COMPOSE_ENV
            .read()
            .expect("Should acquire read lock on DOCKER_COMPOSE_ENV");
        let docker_compose = guard.as_ref().expect("Should have DockerCompose instance");
        let minio_ip = docker_compose.get_container_ip("minio");
        let minio_socket_addr = SocketAddr::new(minio_ip, MINIO_PORT);
        format!("http://{minio_socket_addr}")
    };

    let access_key = std::env::var("MINIO_ACCESS_KEY_ID").unwrap_or("admin".to_string());
    let secret_key = std::env::var("MINIO_SECRET_ACCESS_KEY").unwrap_or("password".to_string());

    HadoopCatalogBuilder::default()
        .with_warehouse_root("s3a://hadoop/")
        .set_property(S3_REGION, "us-east-1")
        .set_property(S3_ENDPOINT, minio_endpoint)
        .set_property(S3_ACCESS_KEY_ID, access_key)
        .set_property(S3_SECRET_ACCESS_KEY, secret_key)
}

#[cfg(feature = "test_hadoop_catalog_docker")]
#[ctor]
#[allow(clippy::expect_used)]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV
        .write()
        .expect("Should acquire write lock on DOCKER_COMPOSE_ENV");
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/tests/hadoop_data", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.down();
    docker_compose.up();
    guard.replace(docker_compose);
}

#[cfg(feature = "test_hadoop_catalog_docker")]
#[dtor]
#[allow(clippy::expect_used)]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV
        .write()
        .expect("Should acquire write lock on DOCKER_COMPOSE_ENV");
    let compose = guard.take();
    if let Some(compose) = compose {
        compose.down();
    }
}

#[allow(clippy::expect_used)]
async fn build_catalogs() -> Vec<(&'static str, HadoopCatalog)> {
    vec![
        #[cfg(feature = "test_hadoop_catalog_docker")]
        (
            "file",
            get_file_hadoop_catalog()
                .build()
                .await
                .expect("Should build file catalog"),
        ),
        (
            "s3a",
            get_s3a_hadoop_catalog()
                .build()
                .await
                .expect("Should build S3A catalog"),
        ),
    ]
}

mod tests {
    use data_components::iceberg::provider::IcebergCatalogProvider;
    use datafusion::catalog::CatalogProvider;
    use globset::{Glob, GlobSet};

    use super::*;

    #[tokio::test]
    async fn test_hadoop_catalog_list_tables() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            // List tables in the test namespace
            let mut tables = catalog
                .list_tables(&iceberg::NamespaceIdent::new("test".to_string()))
                .await
                .expect("Should list tables");
            tables.sort_by(|a, b| a.name().cmp(b.name()));

            assert!(
                tables.len() == 2,
                "{name} - Should have exactly two tables in the test namespace, found: {tables:?}",
            );

            assert!(
                tables[0].name() == "my_table_1",
                "{name} - The table name should be 'my_table_1', found: {}",
                tables[0].name()
            );

            assert!(
                tables[1].name() == "my_table_2",
                "{name} - The table name should be 'my_table_2', found: {}",
                tables[1].name()
            );

            // List tables in the nested namespace
            let mut tables = catalog
                .list_tables(
                    &iceberg::NamespaceIdent::from_strs(["nested", "test"])
                        .expect("Should create namespace"),
                )
                .await
                .expect("Should list tables");
            tables.sort_by(|a, b| a.name().cmp(b.name()));

            assert!(
                tables.len() == 1,
                "{name} - Should have exactly one tables in the test namespace, found: {tables:?}",
            );

            assert!(
                tables[0].name() == "my_table_3",
                "{name} - The table name should be 'my_table_3', found: {}",
                tables[0].name()
            );
        }
    }

    #[tokio::test]
    async fn test_hadoop_catalog_list_namespaces() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            // List namespaces
            let mut namespaces = catalog
                .list_namespaces(None)
                .await
                .expect("Should list namespaces");

            assert!(
                namespaces.len() == 2,
                "{name} - Should have exactly one namespace, found: {namespaces:?}",
            );

            namespaces.sort_by_key(std::string::ToString::to_string);

            assert!(
                namespaces[0].to_string() == "nested",
                "{name} - The namespace should be 'nested', found: {}",
                namespaces[0]
            );

            assert!(
                namespaces[1].to_string() == "test",
                "{name} - The namespace should be 'test', found: {}",
                namespaces[1]
            );

            // List namespaces in the nested namespace
            let nested_namespaces = catalog
                .list_namespaces(Some(&iceberg::NamespaceIdent::new("nested".to_string())))
                .await
                .expect("Should list nested namespaces");

            assert!(
                nested_namespaces.len() == 1,
                "{name} - Should have exactly one nested namespace, found: {nested_namespaces:?}",
            );

            assert!(
                nested_namespaces[0].to_string() == "nested.test",
                "{name} - The nested namespace should be 'nested.test', found: {}",
                nested_namespaces[0]
            );
        }
    }

    #[tokio::test]
    async fn test_hadoop_catalog_namespace_exists() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            // Check if the namespace exists
            let exists = catalog
                .namespace_exists(&iceberg::NamespaceIdent::new("test".to_string()))
                .await
                .expect("Should check namespace existence");

            assert!(exists, "{name} - The namespace 'test' should exist");

            // Check a non-existing namespace
            let non_existing = catalog
                .namespace_exists(&iceberg::NamespaceIdent::new("non_existing".to_string()))
                .await
                .expect("Should check non-existing namespace");

            assert!(
                !non_existing,
                "{name} - The namespace 'non_existing' should not exist"
            );

            // Check a nested namespace
            let nested_exists = catalog
                .namespace_exists(
                    &iceberg::NamespaceIdent::from_strs(["nested", "test"])
                        .expect("Should create nested namespace"),
                )
                .await
                .expect("Should check nested namespace existence");

            assert!(
                nested_exists,
                "{name} - The namespace 'nested.test' should exist"
            );
        }
    }

    #[tokio::test]
    async fn test_hadoop_catalog_table_exists() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            // Check if a table exists
            let exists = catalog
                .table_exists(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::new("test".to_string()),
                    "my_table_1".to_string(),
                ))
                .await
                .expect("Should check table existence");

            assert!(exists, "{name} - The table 'my_table_1' should exist");

            // Check if a nested table exists
            let exists = catalog
                .table_exists(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::from_strs(["nested", "test"])
                        .expect("Should create nested namespace"),
                    "my_table_3".to_string(),
                ))
                .await
                .expect("Should check table existence");

            assert!(exists, "{name} - The table 'my_table_3' should exist");

            // Check a non-existing table
            let non_existing = catalog
                .table_exists(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::new("test".to_string()),
                    "non_existing_table".to_string(),
                ))
                .await
                .expect("Should check non-existing table");

            assert!(
                !non_existing,
                "{name} - The table 'non_existing_table' should not exist"
            );
        }
    }

    #[allow(clippy::expect_used)]
    async fn load_table_and_check_results(
        namespace: NamespaceIdent,
        catalog: &HadoopCatalog,
        catalog_name: &str,
        table_name: &str,
    ) {
        let table = catalog
            .load_table(&iceberg::TableIdent::new(namespace, table_name.to_string()))
            .await
            .expect("Should load table");

        assert!(
            table.identifier().name() == table_name,
            "{catalog_name} - The loaded table name should be '{table_name}', found: {}",
            table.identifier().name()
        );

        // Check if the table is readonly
        assert!(
            table.readonly(),
            "{catalog_name} - The table should be readonly"
        );

        // Read rows from the table
        let table_scan = table
            .scan()
            .select_all()
            .build()
            .expect("Should build scan");
        let mut row_stream = table_scan
            .to_arrow()
            .await
            .expect("Should convert to Arrow");

        let mut row_count = 0;
        let mut row_data: Vec<RecordBatch> = Vec::new();
        while let Some(row) = row_stream.try_next().await.expect("Should read next row") {
            row_count += row.num_rows();
            row_data.push(row);
        }

        // Concat the batches, and order the rows
        let batch = arrow::compute::concat_batches(&row_data[0].schema(), &row_data)
            .expect("Should concatenate batches");
        let column_to_sort = batch.column(0);
        let sorted_indices = arrow::compute::sort_to_indices(column_to_sort, None, None)
            .expect("Should sort indices");
        let sorted_columns: Vec<Arc<dyn arrow_array::Array>> = batch
            .columns()
            .iter()
            .map(|array| {
                arrow::compute::take(array, &sorted_indices, None)
                    .expect("Should take sorted indices")
            })
            .collect();
        let sorted_batch =
            RecordBatch::try_new(batch.schema(), sorted_columns).expect("Should sort batch");

        assert!(
            row_count == 2,
            "{catalog_name} - Should have exactly 2 rows in the table, found: {row_count}",
        );

        let snapshot_content = arrow::util::pretty::pretty_format_batches(&[sorted_batch])
            .expect("Should print batches");

        insta::assert_snapshot!(format!("{catalog_name}_{table_name}"), snapshot_content);
    }

    #[tokio::test]
    async fn test_hadoop_load_table() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            // Load a table - my_table_1
            load_table_and_check_results(
                iceberg::NamespaceIdent::new("test".to_string()),
                &catalog,
                name,
                "my_table_1",
            )
            .await;

            // Load another table - my_table_2
            load_table_and_check_results(
                iceberg::NamespaceIdent::new("test".to_string()),
                &catalog,
                name,
                "my_table_2",
            )
            .await;

            // Load another table - my_table_3
            load_table_and_check_results(
                iceberg::NamespaceIdent::from_strs(["nested", "test"])
                    .expect("Should create nested namespace"),
                &catalog,
                name,
                "my_table_3",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_hadoop_iceberg_provider_includes_tables() {
        let catalogs = build_catalogs().await;

        for (name, catalog) in catalogs {
            let glob_set = GlobSet::builder()
                .add(Glob::new("test.*").expect("Should create Glob"))
                .build()
                .expect("Should build GlobSet");

            let catalog = Arc::new(catalog) as Arc<dyn Catalog>;

            let provider =
                IcebergCatalogProvider::try_new(Arc::clone(&catalog), None, Some(&glob_set))
                    .await
                    .expect("Should create provider");

            // include does not prune namespaces
            assert!(
                provider.schema_names().len() == 2,
                "{name} - Should have exactly two schemas, found: {:?}",
                provider.schema_names()
            );

            let mut namespaces = provider.schema_names();
            namespaces.sort();

            assert_eq!(
                namespaces,
                vec!["nested", "test"],
                "{name} - The schema names should be 'nested' and 'test', found: {namespaces:?}",
            );

            assert!(
                provider
                    .schema("test")
                    .expect("Should get schema")
                    .table_names()
                    .len()
                    == 2,
                "{name} - Should have exactly two tables in the 'test' schema, found: {:?}",
                provider
                    .schema("test")
                    .expect("Should get schema")
                    .table_names()
            );

            let mut table_names = provider
                .schema("test")
                .expect("Should get schema")
                .table_names();
            table_names.sort();

            assert_eq!(
                table_names,
                vec!["my_table_1", "my_table_2"],
                "{name} - The table names in the 'test' schema should be 'my_table_1' and 'my_table_2', found: {table_names:?}",
            );

            // recreate provider with no includes
            let provider = IcebergCatalogProvider::try_new(catalog, None, None)
                .await
                .expect("Should create provider without includes");

            // there should be 2 namespaces now: "nested" and "test"
            assert!(
                provider.schema_names().len() == 2,
                "{name} - Should have exactly two schemas, found: {:?}",
                provider.schema_names()
            );

            let mut namespaces = provider.schema_names();
            namespaces.sort();

            assert_eq!(
                namespaces,
                vec!["nested", "test"],
                "{name} - The schema names should be 'nested' and 'test', found: {namespaces:?}",
            );
        }
    }
}
